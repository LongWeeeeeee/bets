"""Telegram broadcast must stop calling chats that Telegram reports as gone.

Incident (serv1, 08.10.2026, card ingame-6qzf): chats 1179836674 and 312550797
come from `keys.Chat_ids`; Telegram answers them `400 Bad Request: chat not
found` on every broadcast. `send_message` logged "Removing Telegram subscriber
..." and dropped them from the state file, but `_load_telegram_subscribers_state`
re-seeds the list from `keys.Chat_ids` on every load, so the next broadcast
called them again (total_targets=3, delivered=['7543801207']).

The test drives the PUBLIC `functions.send_message` and mocks only the outbound
HTTP (`requests.post`); the 400 body is the captured fixture
`fixtures/telegram_getchat_chat_not_found_20261008.jsonl`
(README beside it has the capture command).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

BASE_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[2]
for _p in (BASE_DIR, REPO_ROOT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import functions  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "telegram_getchat_chat_not_found_20261008.jsonl"

ADMIN = "7543801207"
DEAD_A = "1179836674"
DEAD_B = "312550797"


def _captured_records() -> dict:
    records = {}
    for line in FIXTURE.read_text(encoding="utf-8").splitlines():
        if line.strip():
            item = json.loads(line)
            records[str(item["chat_id"])] = item
    return records


class _Response:
    def __init__(self, status_code: int, body: dict) -> None:
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body)

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise functions.requests.exceptions.HTTPError(
                f"{self.status_code} Client Error: Bad Request for url: "
                "https://api.telegram.org/bot<Token>/sendMessage",
                response=self,
            )

    def json(self):
        return self._body


class _TextResponse:
    """An intermediary's (proxy) answer: status + plain-text body, never JSON."""

    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise functions.requests.exceptions.HTTPError(
                f"{self.status_code} Client Error: Forbidden for url: "
                "https://api.telegram.org/bot<Token>/sendMessage",
                response=self,
            )

    def json(self):
        raise ValueError("Expecting value: line 1 column 1 (char 0)")


class _FakeTelegram:
    """Outbound HTTP only: records sendMessage per chat, serves getUpdates."""

    def __init__(self, dead_chat_ids) -> None:
        self.dead = set(dead_chat_ids)
        self.raise_for = {}  # chat_id -> exception raised by requests.post
        self.text_reply = {}  # chat_id -> (status, plain-text body)
        self.sent = []  # chat_id of every sendMessage attempt, in order
        self.pending_updates = []
        records = _captured_records()
        self._dead_response = next(iter(records.values()))

    def post(self, url, json=None, **_kwargs):  # noqa: A002 - requests signature
        if url.endswith("/getUpdates"):
            updates, self.pending_updates = self.pending_updates, []
            return _Response(200, {"ok": True, "result": updates})
        assert url.endswith("/sendMessage"), url
        chat_id = str(json["chat_id"])
        self.sent.append(chat_id)
        if chat_id in self.raise_for:
            raise self.raise_for[chat_id]
        if chat_id in self.text_reply:
            return _TextResponse(*self.text_reply[chat_id])
        if chat_id in self.dead:
            return _Response(
                self._dead_response["http_status"], dict(self._dead_response["body"])
            )
        return _Response(200, {"ok": True, "result": {"message_id": 1}})

    def attempts(self, chat_id: str) -> int:
        return self.sent.count(chat_id)


@pytest.fixture
def env(monkeypatch, tmp_path):
    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(
        functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False
    )
    monkeypatch.setattr(functions, "TELEGRAM_UPDATES_FETCH_ENABLED", False, raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_IDS", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", ADMIN, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [int(DEAD_A), int(DEAD_B)], raising=False)
    monkeypatch.setattr(functions, "_vk_is_enabled", lambda: False)
    monkeypatch.setattr(functions, "_build_admin_telegram_reply_markup", lambda: None)
    fake = _FakeTelegram({DEAD_A, DEAD_B})
    monkeypatch.setattr(functions.requests, "post", fake.post)
    return {"fake": fake, "state_path": state_path, "legacy_path": legacy_path}


def _state(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _broadcast(text: str = "broadcast"):
    return functions.send_message(text, require_delivery=True, mirror_to_vk=False)


def test_dead_subscribers_from_keys_are_not_called_on_the_next_broadcast(env) -> None:
    fake = env["fake"]

    assert _broadcast("first") is True
    # First broadcast has no knowledge yet: all three configured chats are tried.
    assert sorted(fake.sent) == sorted([ADMIN, DEAD_A, DEAD_B])

    fake.sent.clear()
    assert _broadcast("second") is True
    # Second broadcast must not call the chats Telegram already called gone.
    assert fake.sent == [ADMIN]

    for path in (env["state_path"], env["legacy_path"]):
        saved = _state(path)
        assert sorted(saved["removed_chat_ids"]) == sorted([DEAD_A, DEAD_B])
        assert ADMIN in saved["chat_ids"]
        assert DEAD_A not in saved["chat_ids"]
        assert DEAD_B not in saved["chat_ids"]


def test_tombstones_cover_env_defaults_and_survive_reload(env, monkeypatch) -> None:
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)
    monkeypatch.setenv("TELEGRAM_CHAT_IDS", f"{DEAD_A}, {DEAD_B}")
    fake = env["fake"]

    assert _broadcast("first") is True
    assert sorted(fake.sent) == sorted([ADMIN, DEAD_A, DEAD_B])

    fake.sent.clear()
    assert _broadcast("second") is True
    assert fake.sent == [ADMIN]

    # A fresh load (process restart) still honours the persisted tombstones.
    state = functions._load_telegram_subscribers_state()
    assert state["chat_ids"] == [ADMIN]


def test_admin_chat_is_never_tombstoned(env) -> None:
    fake = env["fake"]
    fake.dead = {ADMIN, DEAD_A}  # misclassified/transient terminal error on the owner chat

    assert _broadcast("first") is True  # DEAD_B still delivers
    assert sorted(fake.sent) == sorted([ADMIN, DEAD_A, DEAD_B])

    saved = _state(env["state_path"])
    assert saved["removed_chat_ids"] == [DEAD_A]
    assert ADMIN not in saved.get("removed_chat_ids", [])

    # Owner chat keeps being tried on the next broadcast, DEAD_A does not.
    fake.sent.clear()
    fake.dead = set()
    assert _broadcast("second") is True
    assert sorted(fake.sent) == sorted([ADMIN, DEAD_B])


def test_getupdates_message_restores_a_tombstoned_chat(env, monkeypatch) -> None:
    fake = env["fake"]
    monkeypatch.setattr(functions, "TELEGRAM_UPDATES_FETCH_ENABLED", True, raising=False)

    assert _broadcast("first") is True
    fake.sent.clear()
    assert _broadcast("second") is True
    assert fake.sent == [ADMIN]
    assert sorted(_state(env["state_path"])["removed_chat_ids"]) == sorted([DEAD_A, DEAD_B])

    # The person writes /start to the bot again and is reachable now.
    fake.dead = {DEAD_A}
    fake.pending_updates = [
        {
            "update_id": 900,
            "message": {
                "message_id": 5,
                "date": 1791500000,
                "text": "/start",
                "chat": {"id": int(DEAD_B), "type": "private"},
                "from": {"id": int(DEAD_B), "is_bot": False},
            },
        }
    ]
    fake.sent.clear()
    assert _broadcast("third") is True
    assert sorted(fake.sent) == sorted([ADMIN, DEAD_B])

    for path in (env["state_path"], env["legacy_path"]):
        saved = _state(path)
        assert saved["removed_chat_ids"] == [DEAD_A]
        assert DEAD_B in saved["chat_ids"]


def test_state_without_removed_chat_ids_is_backward_compatible(env) -> None:
    env["state_path"].write_text(
        json.dumps({"chat_ids": [ADMIN, "555"], "last_update_id": 7}), encoding="utf-8"
    )

    state = functions._load_telegram_subscribers_state()

    assert state["chat_ids"] == [ADMIN, DEAD_A, DEAD_B, "555"]
    assert state["last_update_id"] == 7
    assert not state.get("removed_chat_ids")


def test_all_chats_tombstoned_does_not_fall_back_to_the_defaults(env, monkeypatch) -> None:
    # No admin chat configured: when every remaining chat is gone, the empty
    # target list must not be replaced by the raw defaults (that re-sends to the
    # dead chats).
    monkeypatch.setattr(functions.keys, "Chat_id", "", raising=False)
    fake = env["fake"]

    with pytest.raises(functions.TelegramSendError):
        _broadcast("first")
    assert sorted(fake.sent) == sorted([DEAD_A, DEAD_B])

    fake.sent.clear()
    assert _broadcast("second") is False
    assert fake.sent == []


@pytest.mark.parametrize(
    "text, terminal",
    [
        # prod log 08.10 (base/runtime/cyberscore_sourcetv.log on serv1), token redacted
        ("Telegram send failed: 400 Client Error: Bad Request for url: "
         "https://api.telegram.org/bot<TOKEN>/sendMessage: Bad Request: chat not found", True),
        # Telegram API 403 description shape ("Forbidden: <reason>")
        ("Telegram send failed: 403 Client Error: Forbidden for url: "
         "https://api.telegram.org/bot<TOKEN>/sendMessage: Forbidden: bot can't initiate conversation with a user", True),
        # proxy fallback failure embedded by _recover_telegram_network_send (requests ProxyError text)
        ("Telegram send failed: HTTPSConnectionPool(host='api.telegram.org', port=443): Max retries exceeded "
         "(Caused by ProxyError('Unable to connect to proxy', OSError('Tunnel connection failed: 403 Forbidden')))", False),
        # a proxy answering 403 to the request itself (non-JSON body, no description)
        ("Telegram send failed: 403 Client Error: Forbidden for url: https://api.telegram.org/bot<TOKEN>/sendMessage", False),
    ],
)
def test_terminal_classifier_ignores_proxy_403(text, terminal) -> None:
    assert functions._is_terminal_telegram_chat_error(functions.TelegramSendError(text)) is terminal


# --- round 2 (card ingame-6qzf, astra + Opus review findings) ---------------------------


def _removed(env) -> list:
    return sorted(functions._load_telegram_subscribers_state().get("removed_chat_ids", []))


@pytest.mark.parametrize(
    "tunnel_text",
    [
        # as specified in the review: the proxy refuses the CONNECT tunnel
        "Tunnel connection failed: 403 Forbidden",
        # a proxy that also adds a reason after "Forbidden:" must still not tombstone
        "Tunnel connection failed: 403 Forbidden: proxy authentication required",
    ],
)
def test_proxy_failure_never_tombstones_a_chat(env, monkeypatch, tunnel_text) -> None:
    # Fallbacks off so the transport error itself surfaces from send_message.
    monkeypatch.setattr(functions, "TELEGRAM_SEND_PROXY_FALLBACK_ENABLED", False, raising=False)
    monkeypatch.setattr(functions, "TELEGRAM_SEND_CURL_FALLBACK_ENABLED", False, raising=False)
    fake = env["fake"]
    fake.dead = set()
    for chat_id in (DEAD_A, DEAD_B):
        fake.raise_for[chat_id] = functions.requests.exceptions.ProxyError(
            "Unable to connect to proxy", OSError(tunnel_text)
        )

    assert _broadcast("first") is True  # admin delivers
    assert _broadcast("second") is True

    assert fake.attempts(DEAD_A) == 2
    assert fake.attempts(DEAD_B) == 2
    assert _removed(env) == []


def test_stale_legacy_tombstone_does_not_remove_a_restored_chat(env) -> None:
    # The primary file was written after /start restored DEAD_B; the legacy write failed
    # (only logged), so the legacy copy still carries the old tombstone.
    env["state_path"].write_text(
        json.dumps({"chat_ids": [ADMIN, DEAD_B], "last_update_id": 5, "removed_chat_ids": []}),
        encoding="utf-8",
    )
    env["legacy_path"].write_text(
        json.dumps({"chat_ids": [ADMIN], "last_update_id": 4, "removed_chat_ids": [DEAD_B]}),
        encoding="utf-8",
    )

    state = functions._load_telegram_subscribers_state()

    assert DEAD_B in state["chat_ids"]
    assert DEAD_B not in state["removed_chat_ids"]
    assert state["_needs_persist"] is True  # the differing legacy copy is rewritten on save


def test_legacy_tombstone_still_seeds_when_the_primary_file_is_absent(env) -> None:
    env["legacy_path"].write_text(
        json.dumps({"chat_ids": [ADMIN], "last_update_id": 4, "removed_chat_ids": [DEAD_B]}),
        encoding="utf-8",
    )

    state = functions._load_telegram_subscribers_state()

    assert DEAD_B not in state["chat_ids"]
    assert state["removed_chat_ids"] == [DEAD_B]


def test_null_chat_ids_in_the_state_file_does_not_break_the_broadcast(env) -> None:
    env["state_path"].write_text(
        json.dumps({"chat_ids": None, "last_update_id": 3}), encoding="utf-8"
    )
    fake = env["fake"]

    assert _broadcast("hello") is True  # no TypeError; the admin chat is delivered

    assert fake.attempts(ADMIN) == 1


def test_non_json_403_from_an_intermediary_never_tombstones_a_chat(env) -> None:
    fake = env["fake"]
    fake.dead = set()
    for chat_id in (DEAD_A, DEAD_B):
        fake.text_reply[chat_id] = (403, "Forbidden: access denied")

    assert _broadcast("first") is True
    assert _broadcast("second") is True

    assert fake.attempts(DEAD_A) == 2
    assert fake.attempts(DEAD_B) == 2
    assert _removed(env) == []
