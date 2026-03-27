from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import orjson
import pytest
from bs4 import BeautifulSoup


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import cyberscore_try as runtime  # noqa: E402


class _FakeTextResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code


class _FakeJsonResponse:
    def __init__(self, payload: Dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.text = "{}"

    def json(self) -> Dict[str, Any]:
        return self._payload


def _build_heads_and_bodies():
    html = """
    <div class="head">
      <div class="event__info-info__time">live</div>
    </div>
    <div class="body">
      <div class="match__item-team__score">0</div>
      <div class="match__item-team__score">0</div>
      <a href="https://dltv.org/matches/test-integrity"></a>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    head = soup.find("div", class_="head")
    body = soup.find("div", class_="body")
    assert head is not None and body is not None
    return [head], [body]


def _valid_heroes(seed: int, positions: int = 5) -> Dict[str, Dict[str, int]]:
    pos_order = ["pos1", "pos2", "pos3", "pos4", "pos5"][:positions]
    return {
        pos: {"hero_id": seed + idx + 1, "account_id": seed + idx + 101}
        for idx, pos in enumerate(pos_order)
    }


def test_add_url_creates_json_array_and_deduplicates(tmp_path, monkeypatch) -> None:
    target_path = tmp_path / "map_id_check.json"
    monkeypatch.setattr(runtime, "MAP_ID_CHECK_PATH", str(target_path), raising=False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)

    with runtime.processed_urls_lock:
        runtime.processed_urls_cache.clear()

    runtime.add_url("dltv.org/matches/test-integrity.0", reason="unit_test")
    runtime.add_url("dltv.org/matches/test-integrity.0", reason="unit_test_repeat")

    assert target_path.exists()
    assert orjson.loads(target_path.read_bytes()) == ["dltv.org/matches/test-integrity.0"]


def test_add_url_recovers_corrupt_map_id_check_and_persists_url(tmp_path, monkeypatch) -> None:
    target_path = tmp_path / "map_id_check.json"
    target_path.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(runtime, "MAP_ID_CHECK_PATH", str(target_path), raising=False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)

    with runtime.processed_urls_lock:
        runtime.processed_urls_cache.clear()

    runtime.add_url("dltv.org/matches/test-recover.0", reason="unit_test_recover")

    assert orjson.loads(target_path.read_bytes()) == ["dltv.org/matches/test-recover.0"]
    assert list(tmp_path.glob("map_id_check.json.corrupt.*"))


def test_send_message_requires_delivery_confirmation(monkeypatch) -> None:
    import functions

    class _RejectedResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return {"ok": False, "description": "bot was blocked by the user"}

    monkeypatch.setattr(functions.requests, "post", lambda *_args, **_kwargs: _RejectedResponse())

    with pytest.raises(RuntimeError):
        functions.send_message("test message", require_delivery=True)


def test_send_message_uses_curl_fallback_on_ssl_connection_error(tmp_path, monkeypatch) -> None:
    import functions
    monkeypatch.setattr(functions, "TELEGRAM_UPDATES_FETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", tmp_path / "telegram_subscribers_state.json", raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", tmp_path / "legacy_telegram_subscribers_state.json", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)

    class _CurlResult:
        returncode = 0
        stdout = '{"ok": true, "result": {"message_id": 1}}'
        stderr = ""

    monkeypatch.setattr(
        functions.requests,
        "post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            functions.requests.exceptions.ConnectionError(
                "SSLEOFError(8, 'EOF occurred in violation of protocol')"
            )
        ),
    )
    monkeypatch.setattr(functions.shutil, "which", lambda _name: "/usr/bin/curl")

    calls: List[Dict[str, Any]] = []

    def _fake_run(command, **kwargs):
        calls.append({"command": list(command), **kwargs})
        return _CurlResult()

    monkeypatch.setattr(functions.subprocess, "run", _fake_run)

    assert functions.send_message("fallback message", require_delivery=True) is True
    assert len(calls) == 1
    assert "text@-" in calls[0]["command"]
    assert calls[0]["input"] == "fallback message"


def test_auto_add_to_tier2_does_not_send_telegram_message(monkeypatch) -> None:
    sent_messages: List[str] = []

    monkeypatch.setattr(runtime, "_find_known_team_ids_by_name", lambda *_args, **_kwargs: set())
    monkeypatch.setattr(runtime, "_get_team_tier", lambda *_args, **_kwargs: 3)
    monkeypatch.setattr(runtime, "_append_team_to_tier2_file", lambda *_args, **_kwargs: (True, "astini+5"))
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: sent_messages.append(str(message)))

    ok, resolved_team_id = runtime._ensure_known_team_or_add_to_tier2(
        team_ids=[10081431],
        team_name="Astini+5",
        match_url="dltv.org/matches/test-auto-tier2",
    )

    assert ok is True
    assert resolved_team_id == 10081431
    assert sent_messages == []


def test_send_message_uses_proxy_fallback_before_curl(tmp_path, monkeypatch) -> None:
    import functions
    monkeypatch.setattr(functions, "TELEGRAM_UPDATES_FETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", tmp_path / "telegram_subscribers_state.json", raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", tmp_path / "legacy_telegram_subscribers_state.json", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)

    class _ProxyResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return {"ok": True, "result": {"message_id": 2}}

    post_calls: List[Dict[str, Any]] = []

    def _fake_post(*_args, **kwargs):
        post_calls.append(dict(kwargs))
        if kwargs.get("proxies"):
            return _ProxyResponse()
        raise functions.requests.exceptions.ConnectionError(
            "SSLEOFError(8, 'EOF occurred in violation of protocol')"
        )

    monkeypatch.setattr(functions.requests, "post", _fake_post)
    monkeypatch.setattr(
        functions.keys,
        "BOOKMAKER_PROXIES",
        {"http": "http://proxy.example:8080", "https": "http://proxy.example:8080"},
        raising=False,
    )
    monkeypatch.setattr(functions.subprocess, "run", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("curl should not be used")))

    assert functions.send_message("proxy message", require_delivery=True) is True
    assert len(post_calls) == 2
    assert post_calls[0].get("proxies") in (None, {})
    assert post_calls[1].get("proxies") == {
        "http": "http://proxy.example:8080",
        "https": "http://proxy.example:8080",
    }


def test_send_message_broadcasts_to_discovered_subscribers(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)

    class _Response:
        def __init__(self, payload: Dict[str, Any]) -> None:
            self._payload = payload
            self.status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return self._payload

    delivered: List[str] = []

    def _fake_post(url, **kwargs):
        if url.endswith("/getUpdates"):
            return _Response(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 10,
                            "message": {
                                "chat": {"id": 200},
                                "from": {"id": 200},
                            },
                        }
                    ],
                }
            )
        delivered.append(str(kwargs["json"]["chat_id"]))
        return _Response({"ok": True, "result": {"message_id": 1}})

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    assert functions.send_message("broadcast", require_delivery=True) is True
    assert delivered == ["100", "200"]
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["chat_ids"] == ["100", "200"]
    assert state["last_update_id"] == 10


def test_load_telegram_subscribers_state_merges_primary_and_legacy(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    state_path.write_text(
        json.dumps({"chat_ids": ["100"], "last_update_id": 10}),
        encoding="utf-8",
    )
    legacy_path.write_text(
        json.dumps({"chat_ids": ["200", "100"], "last_update_id": 7}),
        encoding="utf-8",
    )
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)

    state = functions._load_telegram_subscribers_state()

    assert state["chat_ids"] == ["100", "200"]
    assert state["last_update_id"] == 10
    assert state["_needs_persist"] is True

    functions._save_telegram_subscribers_state(state)

    saved_primary = json.loads(state_path.read_text(encoding="utf-8"))
    saved_legacy = json.loads(legacy_path.read_text(encoding="utf-8"))
    assert saved_primary == {"chat_ids": ["100", "200"], "last_update_id": 10}
    assert saved_legacy == saved_primary


def test_send_message_removes_blocked_subscriber(tmp_path, monkeypatch) -> None:
    import functions

    state_path = tmp_path / "telegram_subscribers_state.json"
    legacy_path = tmp_path / "legacy_telegram_subscribers_state.json"
    state_path.write_text(
        json.dumps({"chat_ids": ["100", "200"], "last_update_id": 0}),
        encoding="utf-8",
    )
    monkeypatch.setattr(functions, "TELEGRAM_SUBSCRIBERS_STATE_PATH", state_path, raising=False)
    monkeypatch.setattr(functions, "LEGACY_TELEGRAM_SUBSCRIBERS_STATE_PATH", legacy_path, raising=False)
    monkeypatch.setattr(functions.keys, "Chat_id", "100", raising=False)
    monkeypatch.setattr(functions.keys, "Chat_ids", [], raising=False)

    class _Response:
        def __init__(self, payload: Dict[str, Any]) -> None:
            self._payload = payload
            self.status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> Dict[str, Any]:
            return self._payload

    def _fake_post(url, **kwargs):
        if url.endswith("/getUpdates"):
            return _Response({"ok": True, "result": []})
        chat_id = str(kwargs["json"]["chat_id"])
        if chat_id == "200":
            return _Response({"ok": False, "description": "bot was blocked by the user"})
        return _Response({"ok": True, "result": {"message_id": 1}})

    monkeypatch.setattr(functions.requests, "post", _fake_post)

    assert functions.send_message("broadcast", require_delivery=True) is True
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["chat_ids"] == ["100"]


def test_get_id_to_names_path_uses_runtime_base_dir(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "BASE_DIR", tmp_path, raising=False)
    assert runtime._get_id_to_names_path() == tmp_path / "id_to_names.py"


def test_send_message_keeps_uncertain_when_curl_fallback_fails(monkeypatch) -> None:
    import functions

    class _CurlResult:
        returncode = 35
        stdout = ""
        stderr = "OpenSSL SSL_connect: SSL_ERROR_SYSCALL"

    monkeypatch.setattr(
        functions.requests,
        "post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            functions.requests.exceptions.ConnectionError(
                "SSLEOFError(8, 'EOF occurred in violation of protocol')"
            )
        ),
    )
    monkeypatch.setattr(functions.shutil, "which", lambda _name: "/usr/bin/curl")
    monkeypatch.setattr(functions.subprocess, "run", lambda *_args, **_kwargs: _CurlResult())

    with pytest.raises(functions.TelegramSendError) as exc_info:
        functions.send_message("fallback message", require_delivery=True)

    assert exc_info.value.delivery_uncertain is True


def test_deliver_and_persist_signal_does_not_persist_when_send_fails(tmp_path, monkeypatch) -> None:
    journal_path = tmp_path / "sent_signal_recovery.jsonl"
    add_url_calls: List[Dict[str, Any]] = []

    monkeypatch.setattr(runtime, "SENT_SIGNAL_JOURNAL_PATH", str(journal_path), raising=False)
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("telegram down")),
    )

    def _record_add_url(url: str, reason: str = "unspecified", details: Any = None) -> None:
        add_url_calls.append({"url": url, "reason": reason, "details": details})

    monkeypatch.setattr(runtime, "add_url", _record_add_url)

    with pytest.raises(RuntimeError):
        runtime._deliver_and_persist_signal(
            "dltv.org/matches/test-send-fail.0",
            "message",
            add_url_reason="unit_test_send_fail",
            add_url_details={"status": "draft..."},
        )

    assert add_url_calls == []
    assert not journal_path.exists()


def test_deliver_and_persist_signal_journals_after_persist_failure(tmp_path, monkeypatch) -> None:
    journal_path = tmp_path / "sent_signal_recovery.jsonl"
    monkeypatch.setattr(runtime, "SENT_SIGNAL_JOURNAL_PATH", str(journal_path), raising=False)
    monkeypatch.setattr(runtime, "send_message", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "add_url", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")))
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)

    with runtime.processed_urls_lock:
        runtime.processed_urls_cache.clear()

    runtime._deliver_and_persist_signal(
        "dltv.org/matches/test-journal.0",
        "message",
        add_url_reason="unit_test_journal",
        add_url_details={"status": "draft..."},
    )

    assert journal_path.exists()
    journal_lines = [line for line in journal_path.read_bytes().splitlines() if line.strip()]
    assert len(journal_lines) == 1
    payload = orjson.loads(journal_lines[0])
    assert payload["url"] == "dltv.org/matches/test-journal.0"
    assert payload["reason"] == "unit_test_journal"
    assert payload["details"]["persist_error"] == "disk full"
    with runtime.processed_urls_lock:
        assert "dltv.org/matches/test-journal.0" in runtime.processed_urls_cache


def test_deliver_and_persist_signal_uses_fallback_journal_when_primary_unavailable(tmp_path, monkeypatch) -> None:
    primary_path = tmp_path / "sent_signal_recovery.jsonl"
    fallback_path = tmp_path / "sent_signal_recovery_fallback.jsonl"
    monkeypatch.setattr(runtime, "SENT_SIGNAL_JOURNAL_PATH", str(primary_path), raising=False)
    monkeypatch.setattr(runtime, "SENT_SIGNAL_JOURNAL_FALLBACK_PATH", str(fallback_path), raising=False)
    monkeypatch.setattr(runtime, "send_message", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "add_url", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full")))
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)

    original_append = runtime._append_journal_entry_to_path

    def _append_with_primary_failure(path: Path, entry: Dict[str, Any]) -> None:
        if path == primary_path:
            raise OSError("primary journal unavailable")
        original_append(path, entry)

    monkeypatch.setattr(runtime, "_append_journal_entry_to_path", _append_with_primary_failure)

    runtime._deliver_and_persist_signal(
        "dltv.org/matches/test-fallback-journal.0",
        "message",
        add_url_reason="unit_test_fallback_journal",
        add_url_details={"status": "draft..."},
    )

    assert not primary_path.exists()
    fallback_lines = [line for line in fallback_path.read_bytes().splitlines() if line.strip()]
    assert len(fallback_lines) == 1
    payload = orjson.loads(fallback_lines[0])
    assert payload["url"] == "dltv.org/matches/test-fallback-journal.0"


def test_safe_flush_sent_signal_journal_into_map_id_check_swallows_exception(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "_flush_sent_signal_journal_into_map_id_check",
        lambda: (_ for _ in ()).throw(RuntimeError("flush broken")),
    )

    assert runtime._safe_flush_sent_signal_journal_into_map_id_check() == 0


def test_try_acquire_runtime_instance_lock_rejects_busy_lock(tmp_path, monkeypatch) -> None:
    class _BusyFcntl:
        LOCK_EX = 1
        LOCK_NB = 2
        LOCK_UN = 8

        @staticmethod
        def flock(_fd: int, _mode: int) -> None:
            raise OSError("lock busy")

    monkeypatch.setattr(runtime, "RUNTIME_INSTANCE_LOCK_PATH", str(tmp_path / "runtime.lock"), raising=False)
    monkeypatch.setattr(runtime, "fcntl", _BusyFcntl(), raising=False)
    monkeypatch.setattr(runtime, "runtime_instance_lock_handle", None, raising=False)

    assert runtime._try_acquire_runtime_instance_lock(mode_label="no_odds") is False


def test_runtime_instance_lock_path_is_split_by_mode(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "RUNTIME_INSTANCE_LOCK_PATH", str(tmp_path / "runtime.lock"), raising=False)

    no_odds_lock = runtime._runtime_instance_lock_path_for_mode("no_odds")
    odds_lock = runtime._runtime_instance_lock_path_for_mode("odds")

    assert no_odds_lock.name == "runtime.no_odds.lock"
    assert odds_lock.name == "runtime.odds.lock"
    assert no_odds_lock != odds_lock


def test_delayed_queue_path_is_split_by_mode(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(tmp_path / "delayed_signal_queue.json"), raising=False)

    no_odds_queue = runtime._delayed_queue_path_for_mode("no_odds")
    odds_queue = runtime._delayed_queue_path_for_mode("odds")

    assert no_odds_queue.name == "delayed_signal_queue.no_odds.json"
    assert odds_queue.name == "delayed_signal_queue.odds.json"
    assert no_odds_queue != odds_queue


def test_set_delayed_match_persists_and_restores_queue(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()

    runtime._set_delayed_match(
        "dltv.org/matches/test-delayed.0",
        {
            "message": "payload",
            "reason": "late_only",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 100.0,
            "queued_game_time": 700.0,
            "last_game_time": 700.0,
            "last_progress_at": 100.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {},
            "fallback_send_status_label": "late_fallback_20_20_send",
            "allow_live_recheck": False,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    restored = runtime._load_delayed_queue_state(recover=True)

    assert delayed_queue_path.exists()
    assert "dltv.org/matches/test-delayed.0" in restored
    assert restored["dltv.org/matches/test-delayed.0"]["target_game_time"] == float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME)


def test_deliver_and_persist_signal_records_uncertain_delivery_and_blocks_retries(tmp_path, monkeypatch) -> None:
    uncertain_path = tmp_path / "uncertain_signal_delivery.jsonl"
    fallback_path = tmp_path / "uncertain_signal_delivery_fallback.jsonl"
    monkeypatch.setattr(runtime, "UNCERTAIN_SIGNAL_DELIVERY_PATH", str(uncertain_path), raising=False)
    monkeypatch.setattr(runtime, "UNCERTAIN_SIGNAL_DELIVERY_FALLBACK_PATH", str(fallback_path), raising=False)
    monkeypatch.setattr(
        runtime,
        "send_message",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            runtime.TelegramSendError("read timeout", delivery_uncertain=True)
        ),
    )

    add_url_calls: List[str] = []
    monkeypatch.setattr(runtime, "add_url", lambda url, **_kwargs: add_url_calls.append(url))

    with runtime.uncertain_delivery_urls_lock:
        runtime.uncertain_delivery_urls_cache.clear()
    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
        runtime.monitored_matches["dltv.org/matches/test-uncertain.0"] = {"message": "queued"}

    delivered = runtime._deliver_and_persist_signal(
        "dltv.org/matches/test-uncertain.0",
        "message",
        add_url_reason="unit_test_uncertain",
        add_url_details={"status": "draft..."},
    )

    assert delivered is False
    assert add_url_calls == []
    assert uncertain_path.exists()
    lines = [line for line in uncertain_path.read_bytes().splitlines() if line.strip()]
    assert len(lines) == 1
    payload = orjson.loads(lines[0])
    assert payload["url"] == "dltv.org/matches/test-uncertain.0"
    assert runtime._is_url_uncertain_delivery("dltv.org/matches/test-uncertain.0") is True
    with runtime.monitored_matches_lock:
        assert "dltv.org/matches/test-uncertain.0" not in runtime.monitored_matches


def test_get_heads_sets_missing_live_matches_reason_without_telegram(monkeypatch) -> None:
    send_calls: List[str] = []

    monkeypatch.setattr(runtime, "USE_PROXY", True, raising=False)
    monkeypatch.setattr(runtime, "PROXY_LIST", ["proxy-a", "proxy-b"], raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY_INDEX", 0, raising=False)
    monkeypatch.setattr(runtime, "CURRENT_PROXY", "proxy-a", raising=False)
    monkeypatch.setattr(runtime, "PROXIES", {"http": "proxy-a", "https": "proxy-a"}, raising=False)
    monkeypatch.setattr(runtime.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: send_calls.append(str(message)))

    def _fake_retry(*_args, **_kwargs):
        return _FakeTextResponse("<html><body>no live matches here</body></html>")

    monkeypatch.setattr(runtime, "make_request_with_retry", _fake_retry)

    heads, bodies = runtime.get_heads(
        response=_FakeTextResponse("<html><body>no live matches here</body></html>")
    )

    assert heads is None and bodies is None
    assert send_calls == []
    assert (
        runtime.GET_HEADS_LAST_FAILURE_REASON
        == runtime.GET_HEADS_FAILURE_REASON_LIVE_MATCHES_MISSING_ALL_PROXIES
    )


def test_general_notifies_live_matches_missing_only_after_all_proxies(monkeypatch) -> None:
    send_calls: List[str] = []

    monkeypatch.setattr(runtime, "_load_stats_dicts", lambda: None)
    monkeypatch.setattr(runtime, "_safe_flush_sent_signal_journal_into_map_id_check", lambda: 0)
    monkeypatch.setattr(runtime, "_load_map_id_check_urls", lambda recover=True: [])
    monkeypatch.setattr(runtime, "_load_delayed_queue_state", lambda recover=True: {})
    monkeypatch.setattr(runtime, "_replace_monitored_matches_from_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_sync_processed_urls_cache", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_load_uncertain_delivery_urls", lambda: [])
    monkeypatch.setattr(runtime, "_sync_uncertain_delivery_urls_cache", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_ensure_bookmaker_prefetch_started", lambda: None)
    monkeypatch.setattr(runtime, "_stop_bookmaker_prefetch_worker", lambda: None)
    monkeypatch.setattr(runtime, "_init_proxy_pool", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: send_calls.append(str(message)))

    def _heads_request_failed():
        runtime.GET_HEADS_LAST_FAILURE_REASON = runtime.GET_HEADS_FAILURE_REASON_REQUEST_FAILED
        return None, None

    monkeypatch.setattr(runtime, "get_heads", _heads_request_failed)
    assert runtime.general(use_proxy=False, odds=False) is None
    assert send_calls == []

    def _heads_missing_after_all_proxies():
        runtime.GET_HEADS_LAST_FAILURE_REASON = runtime.GET_HEADS_FAILURE_REASON_LIVE_MATCHES_MISSING_ALL_PROXIES
        return None, None

    monkeypatch.setattr(runtime, "get_heads", _heads_missing_after_all_proxies)
    assert runtime.general(use_proxy=False, odds=False) is None
    assert send_calls == ["❌ Не найден элемент live__matches в HTML"]


def test_delayed_send_failure_schedules_backoff(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "DELAYED_SIGNAL_RETRY_BACKOFF_BASE_SECONDS", 30, raising=False)
    monkeypatch.setattr(runtime, "DELAYED_SIGNAL_RETRY_BACKOFF_MAX_SECONDS", 120, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME), "radiant_lead": 0.0},
    )
    monkeypatch.setattr(
        runtime,
        "_deliver_and_persist_signal",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("telegram down")),
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-retry.0",
        {
            "message": "payload",
            "reason": "late_only",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": 1200.0,
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 1100.0,
            "last_game_time": 1100.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {},
            "fallback_send_status_label": "late_fallback_20_20_send",
            "allow_live_recheck": False,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    with runtime.monitored_matches_lock:
        payload = dict(runtime.monitored_matches["dltv.org/matches/test-retry.0"])

    assert payload["retry_attempt_count"] == 1
    assert payload["last_send_error"] == "telegram down"
    assert payload["next_retry_at"] == 1_700_000_030.0


def test_delayed_early_core_timeout_rejects_without_send(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME), "radiant_lead": 0.0},
    )

    send_calls: List[str] = []
    deliver_calls: List[Dict[str, Any]] = []
    add_url_calls: List[Dict[str, Any]] = []
    def _deliver(*args, **kwargs):
        send_calls.append("send")
        deliver_calls.append({"args": args, "kwargs": kwargs})
        return True
    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", _deliver)
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, **kwargs: add_url_calls.append({"url": url, **kwargs}),
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-early-core-timeout.0",
        {
            "message": "payload",
            "reason": "early_star_late_core_wait_1500",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 1100.0,
            "last_game_time": 1100.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "draft..."},
            "fallback_send_status_label": "early_core_fallback_20_20_send",
            "send_on_target_game_time": False,
            "timeout_add_url_reason": "star_signal_rejected_early_core_monitor_timeout",
            "timeout_status_label": "early_core_timeout_no_send",
            "allow_live_recheck": True,
            "networth_monitor_threshold": 1500.0,
            "networth_monitor_deadline_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "networth_target_side": "radiant",
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert send_calls == []
    assert add_url_calls
    assert add_url_calls[-1]["reason"] == "star_signal_rejected_early_core_monitor_timeout"
    assert add_url_calls[-1]["details"]["dispatch_status_label"] == "early_core_timeout_no_send"
    with runtime.monitored_matches_lock:
        assert "dltv.org/matches/test-early-core-timeout.0" not in runtime.monitored_matches


def test_delayed_fallback_uses_post_target_comeback_ceiling(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_thresholds", {"20": 13500.0, "21": 13698.0}, raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_max_minute", 21, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME), "radiant_lead": -3501.0},
    )

    send_calls: List[str] = []
    deliver_calls: List[Dict[str, Any]] = []
    add_url_calls: List[Dict[str, Any]] = []

    def _deliver(*args, **kwargs):
        send_calls.append("send")
        deliver_calls.append({"args": args, "kwargs": kwargs})
        return True

    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", _deliver)
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, **kwargs: add_url_calls.append({"url": url, **kwargs}),
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-late-fallback-guard.0",
        {
            "message": "payload",
            "reason": "late_only_opposite_signs",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 900.0,
            "last_game_time": 900.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "draft...", "target_side": "radiant"},
            "fallback_send_status_label": "late_fallback_20_20_send",
            "fallback_max_deficit_abs": 3000.0,
            "send_on_target_game_time": True,
            "allow_live_recheck": False,
            "networth_target_side": "radiant",
            "late_comeback_monitor_candidate": True,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert send_calls == ["send"]
    assert add_url_calls == []
    assert deliver_calls
    add_url_details = deliver_calls[-1]["kwargs"]["add_url_details"]
    assert add_url_details["late_comeback_monitor_reached"] is True
    assert add_url_details["target_networth_diff"] == pytest.approx(-3501.0)
    assert add_url_details["late_comeback_monitor_minute"] == 20
    assert add_url_details["late_comeback_monitor_threshold"] == pytest.approx(13500.0)
    with runtime.monitored_matches_lock:
        assert "dltv.org/matches/test-late-fallback-guard.0" in runtime.monitored_matches


def test_delayed_late_core_monitor_uses_post_target_comeback_ceiling(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_thresholds", {"20": 13500.0, "21": 13698.0}, raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_max_minute", 21, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME), "radiant_lead": -5000.0},
    )

    send_calls: List[str] = []
    deliver_calls: List[Dict[str, Any]] = []
    add_url_calls: List[Dict[str, Any]] = []

    def _deliver(*args, **kwargs):
        send_calls.append("send")
        deliver_calls.append({"args": args, "kwargs": kwargs})
        return True

    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", _deliver)
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, **kwargs: add_url_calls.append({"url": url, **kwargs}),
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-late-core-post-target.0",
        {
            "message": "payload",
            "reason": "late_star_early_core_wait_800",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 900.0,
            "last_game_time": 900.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "draft...", "target_side": "radiant"},
            "send_on_target_game_time": False,
            "allow_live_recheck": True,
            "networth_monitor_threshold": 800.0,
            "networth_monitor_deadline_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "networth_target_side": "radiant",
            "late_comeback_monitor_candidate": True,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert send_calls == ["send"]
    assert add_url_calls == []
    assert deliver_calls
    add_url_details = deliver_calls[-1]["kwargs"]["add_url_details"]
    assert add_url_details["late_comeback_monitor_reached"] is True
    assert add_url_details["target_networth_diff"] == pytest.approx(-5000.0)
    assert add_url_details["late_comeback_monitor_minute"] == 20
    assert add_url_details["late_comeback_monitor_threshold"] == pytest.approx(13500.0)


def test_delayed_fallback_transitions_into_post_target_comeback_monitor(tmp_path, monkeypatch) -> None:
    delayed_queue_path = tmp_path / "delayed_signal_queue.json"
    monkeypatch.setattr(runtime, "DELAYED_QUEUE_PATH", str(delayed_queue_path), raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_thresholds", {"20": 13500.0, "21": 13698.0}, raising=False)
    monkeypatch.setattr(runtime, "late_comeback_ceiling_max_minute", 21, raising=False)
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_fetch_delayed_match_state",
        lambda _json_url: {"game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME), "radiant_lead": -14000.0},
    )

    send_calls: List[str] = []
    add_url_calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(runtime, "_deliver_and_persist_signal", lambda *_args, **_kwargs: send_calls.append("send"))
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, **kwargs: add_url_calls.append({"url": url, **kwargs}),
    )

    with runtime.monitored_matches_lock:
        runtime.monitored_matches.clear()
    runtime._set_delayed_match(
        "dltv.org/matches/test-post-target-comeback-monitor.0",
        {
            "message": "payload",
            "reason": "late_only_opposite_signs",
            "json_url": "https://dltv.org/live/test.json",
            "target_game_time": float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME),
            "queued_at": 1_699_999_000.0,
            "queued_game_time": 900.0,
            "last_game_time": 900.0,
            "last_progress_at": 1_699_999_000.0,
            "add_url_reason": "star_signal_sent_delayed",
            "add_url_details": {"status": "draft...", "target_side": "radiant"},
            "fallback_send_status_label": "late_fallback_20_20_send",
            "fallback_max_deficit_abs": 3000.0,
            "send_on_target_game_time": True,
            "allow_live_recheck": False,
            "networth_target_side": "radiant",
            "late_comeback_monitor_candidate": True,
            "retry_attempt_count": 0,
            "next_retry_at": 0.0,
        },
    )

    runtime._drain_due_delayed_signals_once()

    assert send_calls == []
    assert add_url_calls == []
    with runtime.monitored_matches_lock:
        payload = dict(runtime.monitored_matches["dltv.org/matches/test-post-target-comeback-monitor.0"])
    assert payload["reason"] == "post_target_comeback_ceiling_monitor"
    assert payload["dispatch_status_label"] == "late_comeback_monitor_wait"
    assert payload["send_on_target_game_time"] is False
    assert payload["late_comeback_monitor_active"] is True
    assert payload["target_game_time"] > float(runtime.DELAYED_SIGNAL_TARGET_GAME_TIME)


def test_legacy_functions_add_url_is_disabled() -> None:
    import functions

    with pytest.raises(RuntimeError):
        functions.add_url("dltv.org/matches/legacy.0")


def test_general_recovers_corrupt_map_id_and_isolates_match_errors(tmp_path, monkeypatch) -> None:
    target_path = tmp_path / "map_id_check.json"
    target_path.write_text("{broken", encoding="utf-8")
    journal_path = tmp_path / "sent_signal_recovery.jsonl"
    monkeypatch.setattr(runtime, "MAP_ID_CHECK_PATH", str(target_path), raising=False)
    monkeypatch.setattr(runtime, "SENT_SIGNAL_JOURNAL_PATH", str(journal_path), raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "USE_PROXY", False, raising=False)
    monkeypatch.setattr(runtime, "_init_proxy_pool", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_load_stats_dicts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_stop_bookmaker_prefetch_worker", lambda *_args, **_kwargs: None)

    heads, bodies = _build_heads_and_bodies()
    monkeypatch.setattr(runtime, "get_heads", lambda *_args, **_kwargs: (heads * 2, bodies * 2))

    processed_indexes: List[int] = []

    def _fake_check_head(_heads, _bodies, i, _maps_data, return_status=None):
        if i == 0:
            raise RuntimeError("boom")
        processed_indexes.append(i)
        return "draft..."

    monkeypatch.setattr(runtime, "check_head", _fake_check_head)

    status = runtime.general(use_proxy=False, odds=False)

    assert status == "draft..."
    assert processed_indexes == [1]
    assert orjson.loads(target_path.read_bytes()) == []
    assert list(tmp_path.glob("map_id_check.json.corrupt.*"))


def test_check_head_skips_invalid_draft_before_synergy(monkeypatch) -> None:
    heads, bodies = _build_heads_and_bodies()
    sent_messages: List[str] = []
    add_url_calls: List[Dict[str, Any]] = []
    synergy_called = {"value": False}

    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: sent_messages.append(str(message)))

    def _record_add_url(url: str, reason: str = "unspecified", details: Any = None):
        add_url_calls.append(
            {
                "url": url,
                "reason": reason,
                "details": dict(details) if isinstance(details, dict) else details,
            }
        )

    monkeypatch.setattr(runtime, "add_url", _record_add_url)

    page_html = "<html><script>$.get('/live/test-integrity.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Radiant Team", "team_id": 1001, "id": 1001},
            "second_team": {"title": "Dire Team", "team_id": 2002, "id": 2002},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
        },
        "radiant_lead": 0.0,
        "game_time": float(10 * 60),
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        runtime,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(
        runtime,
        "parse_draft_and_positions",
        lambda *_args, **_kwargs: (
            _valid_heroes(0, positions=5),
            _valid_heroes(100, positions=4),
            None,
            "",
            [],
        ),
    )

    def _should_not_run(*_args, **_kwargs):
        synergy_called["value"] = True
        raise AssertionError("synergy_and_counterpick must not run for invalid draft")

    monkeypatch.setattr(runtime, "synergy_and_counterpick", _should_not_run)

    runtime.check_head(
        heads=heads,
        bodies=bodies,
        i=0,
        maps_data=set(),
        return_status=None,
    )

    assert synergy_called["value"] is False
    assert sent_messages == []
    assert add_url_calls == []


def test_problem_candidates_are_shown_without_odds(monkeypatch) -> None:
    heads, bodies = _build_heads_and_bodies()
    sent_messages: List[str] = []

    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: sent_messages.append(str(message)))
    monkeypatch.setattr(runtime, "add_url", lambda *_args, **_kwargs: None)

    page_html = "<html><script>$.get('/live/test-problems.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Radiant Team", "team_id": 1001, "id": 1001},
            "second_team": {"title": "Dire Team", "team_id": 2002, "id": 2002},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
        },
        "radiant_lead": 0.0,
        "game_time": float(10 * 60),
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        runtime,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(
        runtime,
        "parse_draft_and_positions",
        lambda *_args, **_kwargs: (
            _valid_heroes(0, positions=5),
            _valid_heroes(100, positions=5),
            None,
            "",
            [
                {"team_key": "radiant", "position": "pos1", "hero_id": 1, "hero_name": "Anti-Mage", "score": 10},
                {"team_key": "dire", "position": "pos5", "hero_id": 50, "hero_name": "Dazzle", "score": 20},
            ],
        ),
    )
    monkeypatch.setattr(
        runtime,
        "synergy_and_counterpick",
        lambda *_args, **_kwargs: {"early_output": {"solo": 3}, "mid_output": {"solo": 3}},
    )
    monkeypatch.setattr(runtime, "calculate_lanes", lambda *_args, **_kwargs: ("", "", ""))
    monkeypatch.setattr(runtime, "format_output_dict", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        runtime,
        "_star_block_diagnostics",
        lambda *, raw_block, target_wr, section: {
            "valid": True,
            "status": "ok",
            "sign": 1,
            "hit_metrics": ["solo"],
            "conflict_metric": None,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_block_signs_same_or_zero",
        lambda *_args, **_kwargs: {"valid": True, "status": "ok"},
    )
    monkeypatch.setattr(runtime, "_format_raw_star_block_metrics", lambda *_args, **_kwargs: "none")
    monkeypatch.setattr(runtime, "_decorate_star_block_for_display", lambda raw_block, **_kwargs: dict(raw_block or {}))
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)

    runtime.check_head(
        heads=heads,
        bodies=bodies,
        i=0,
        maps_data=set(),
        return_status=None,
    )

    assert sent_messages, "Expected a sent message"
    assert "problem_positions_top2" in sent_messages[0]
    

def test_team_elo_block_is_shown_in_telegram_message(monkeypatch) -> None:
    heads, bodies = _build_heads_and_bodies()
    sent_messages: List[str] = []

    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda message, **_kwargs: sent_messages.append(str(message)))
    monkeypatch.setattr(runtime, "add_url", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "_build_team_elo_matchup_summary",
        lambda *_args, **_kwargs: {
            "radiant": {"rating": 1655.0, "base_rating": 1655.0, "games": 42},
            "dire": {"rating": 1570.0, "base_rating": 1570.0, "games": 37},
            "radiant_win_prob": 0.619,
            "dire_win_prob": 0.381,
            "elo_diff": 85.0,
        },
    )

    page_html = "<html><script>$.get('/live/test-elo.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Radiant Team", "team_id": 1001, "id": 1001},
            "second_team": {"title": "Dire Team", "team_id": 2002, "id": 2002},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
        },
        "radiant_lead": 0.0,
        "game_time": float(10 * 60),
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        runtime,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(
        runtime,
        "parse_draft_and_positions",
        lambda *_args, **_kwargs: (
            _valid_heroes(0, positions=5),
            _valid_heroes(100, positions=5),
            None,
            "",
            [],
        ),
    )
    monkeypatch.setattr(
        runtime,
        "synergy_and_counterpick",
        lambda *_args, **_kwargs: {"early_output": {"solo": 3}, "mid_output": {"solo": 3}},
    )
    monkeypatch.setattr(runtime, "calculate_lanes", lambda *_args, **_kwargs: ("", "", ""))
    monkeypatch.setattr(runtime, "format_output_dict", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        runtime,
        "_star_block_diagnostics",
        lambda *, raw_block, target_wr, section: {
            "valid": True,
            "status": "ok",
            "sign": 1,
            "hit_metrics": ["solo"],
            "conflict_metric": None,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_block_signs_same_or_zero",
        lambda *_args, **_kwargs: {"valid": True, "status": "ok"},
    )
    monkeypatch.setattr(runtime, "_format_raw_star_block_metrics", lambda *_args, **_kwargs: "none")
    monkeypatch.setattr(runtime, "_decorate_star_block_for_display", lambda raw_block, **_kwargs: dict(raw_block or {}))
    monkeypatch.setattr(runtime.time, "time", lambda: 1_700_000_000.0)

    runtime.check_head(
        heads=heads,
        bodies=bodies,
        i=0,
        maps_data=set(),
        return_status=None,
    )

    assert sent_messages, "Expected a sent message"
    assert "Командный ELO:" in sent_messages[0]
    assert "Radiant Team: 1655" in sent_messages[0]
    assert "Dire Team: 1570" in sent_messages[0]
    assert "ELO WR≈61.9% / 38.1% (ΔELO +85)" in sent_messages[0]


def test_team_elo_block_separates_raw_team_elo_from_tier_adjusted_matchup(monkeypatch) -> None:
    block, _meta = runtime._format_team_elo_block(
        {
            "radiant": {"rating": 1593.3, "base_rating": 1511.4},
            "dire": {"rating": 1485.4, "base_rating": 1567.3},
            "radiant_win_prob": 0.6505,
            "dire_win_prob": 0.3495,
            "elo_diff": 107.9,
            "tier_gap_bonus": 163.9,
            "tier_gap_key": "TIER1_vs_TIER2",
        },
        radiant_team_name="L1GA TEAM",
        dire_team_name="Pipsqueak+4",
    )

    assert "L1GA TEAM: 1511" in block
    assert "Pipsqueak+4: 1567" in block
    assert "Raw WR≈42.0% / 58.0% (ΔELO -56)" in block
    assert "Adj WR≈65.0% / 34.9% (ΔELO +108, tier bonus +164 TIER1_vs_TIER2)" in block


def test_team_elo_block_marks_current_lineup_source() -> None:
    block, meta = runtime._format_team_elo_block(
        {
            "source": "elo_live_lineup_snapshot",
            "radiant": {"rating": 1600.0, "base_rating": 1600.0, "lineup_used": True},
            "dire": {"rating": 1500.0, "base_rating": 1500.0, "lineup_used": False},
            "radiant_win_prob": 0.6401,
            "dire_win_prob": 0.3599,
            "elo_diff": 100.0,
        },
        radiant_team_name="L1GA TEAM",
        dire_team_name="Astini+5",
    )

    assert "Командный ELO (текущий состав):" in block
    assert "L1GA TEAM: 1600" in block
    assert "Astini+5: 1500" in block
    assert isinstance(meta, dict)
    assert meta["lineup_used"] is True
    assert meta["source"] == "elo_live_lineup_snapshot"


def test_team_elo_block_shows_live_delta_vs_snapshot() -> None:
    block, meta = runtime._format_team_elo_block(
        {
            "source": "elo_live_lineup_snapshot",
            "radiant": {
                "rating": 1608.0,
                "base_rating": 1608.0,
                "snapshot_base_rating": 1600.0,
                "live_base_delta": 8.0,
                "lineup_used": True,
            },
            "dire": {
                "rating": 1491.0,
                "base_rating": 1491.0,
                "snapshot_base_rating": 1500.0,
                "live_base_delta": -9.0,
                "lineup_used": True,
            },
            "radiant_win_prob": 0.6621,
            "dire_win_prob": 0.3379,
            "elo_diff": 117.0,
        },
        radiant_team_name="Nemiga Gaming",
        dire_team_name="Spirit Academy",
    )

    assert "Δ live vs snapshot: +8 / -9" in block
    assert isinstance(meta, dict)
    assert meta["radiant_live_base_delta"] == pytest.approx(8.0)
    assert meta["dire_live_base_delta"] == pytest.approx(-9.0)


def test_bookmaker_odds_block_shows_match_fallback_row(monkeypatch) -> None:
    snapshot = {
        "status": "done",
        "mode": "live",
        "map_num": 2,
        "sites": {
            "betboom": {"odds": [], "match_odds": [], "market_closed": False},
            "pari": {"odds": [1.58, 2.25], "match_odds": [], "market_closed": False},
            "winline": {"odds": [], "match_odds": [1.30, 3.15], "market_closed": False},
        },
    }
    monkeypatch.setattr(runtime, "_bookmaker_prefetch_lookup", lambda *_args, **_kwargs: snapshot)

    block, ready, reason = runtime._bookmaker_format_odds_block("https://example.com/match")

    assert ready is True
    assert reason == "ok"
    assert "Букмекеры (live, карта 2):" in block
    assert "Pari: П1 1.58 | П2 2.25" in block
    assert "Winline (матч): П1 1.30 | П2 3.15" in block


@pytest.mark.parametrize("module_name", ["functions", "signal_wrappers"])
def test_partial_star_threshold_sections_do_not_fallback_to_wr60(tmp_path, monkeypatch, module_name) -> None:
    module = __import__(module_name)
    thresholds_path = tmp_path / f"{module_name}_thresholds.json"
    thresholds_path.write_text(
        """
        {
          "60": {
            "early_output": [["solo", 3]],
            "mid_output": [["solo", 3]]
          },
          "65": {
            "early_output": [["solo", 5]],
            "mid_output": []
          }
        }
        """.strip(),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "STAR_THRESHOLDS_PATH", thresholds_path, raising=False)
    if hasattr(module._load_star_thresholds, "cache_clear"):
        module._load_star_thresholds.cache_clear()

    loaded = module._load_star_thresholds()

    assert loaded[65]["early_output"] == [("solo", 5)]
    assert loaded[65]["mid_output"] == []
    if hasattr(module._load_star_thresholds, "cache_clear"):
        module._load_star_thresholds.cache_clear()


@pytest.mark.parametrize("module_name", ["functions", "signal_wrappers"])
def test_malformed_star_threshold_file_raises(tmp_path, monkeypatch, module_name) -> None:
    module = __import__(module_name)
    thresholds_path = tmp_path / f"{module_name}_thresholds_invalid.json"
    thresholds_path.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(module, "STAR_THRESHOLDS_PATH", thresholds_path, raising=False)
    if hasattr(module._load_star_thresholds, "cache_clear"):
        module._load_star_thresholds.cache_clear()

    with pytest.raises(RuntimeError):
        module._load_star_thresholds()

    if hasattr(module._load_star_thresholds, "cache_clear"):
        module._load_star_thresholds.cache_clear()


def test_format_output_dict_does_not_fallback_to_wr60_when_target_missing(monkeypatch) -> None:
    import functions

    monkeypatch.setattr(
        functions,
        "STAR_THRESHOLDS_BY_WR",
        {
            60: {
                "early_output": [("solo", 3)],
                "mid_output": [("solo", 3)],
            }
        },
        raising=False,
    )

    has_star = functions.format_output_dict(
        {"early_output": {"solo": 3}, "mid_output": {"solo": 3}},
        target_wr=65,
        late_signal_gate_enabled=False,
    )

    assert has_star is False


def test_finalize_orphaned_live_elo_series_uses_finished_page_score(tmp_path, monkeypatch) -> None:
    progress_path = tmp_path / "live_elo_progress.json"
    progress_path.write_text(
        json.dumps(
            {
                "pending_series": {
                    "425561": {
                        "series_key": "425561",
                        "series_url": "dltv.org/matches/425561/pipsqueak4-vs-l1ga-team-premier-series-play-in",
                        "last_scores": {"first": 0, "second": 1},
                        "pending_map": {
                            "map_key": "dltv.org/matches/425561/pipsqueak4-vs-l1ga-team-premier-series-play-in.1",
                            "registered_at": 0,
                        },
                        "updated_at": 0,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(runtime, "ELO_LIVE_SNAPSHOT_AVAILABLE", True, raising=False)
    monkeypatch.setattr(runtime, "_elo_live_finalize_series_from_scores", object(), raising=False)
    monkeypatch.setattr(runtime, "_elo_live_default_progress_path", progress_path, raising=False)
    monkeypatch.setattr(runtime, "LIVE_ELO_ORPHAN_PENDING_MIN_AGE_SECONDS", 0, raising=False)
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(
            "<html><head><title>Pipsqueak+4 0-2 L1ga Team (Mar. 21, 2026) Final Score - DLTV</title></head></html>"
        ),
        raising=False,
    )

    finalize_calls: List[Dict[str, Any]] = []

    def _fake_finalize_finished_live_series_for_elo(**kwargs):
        finalize_calls.append(dict(kwargs))
        return {
            "applied_update": {
                "map_key": "dltv.org/matches/425561/pipsqueak4-vs-l1ga-team-premier-series-play-in.1"
            }
        }

    dropped: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        runtime,
        "_finalize_finished_live_series_for_elo",
        _fake_finalize_finished_live_series_for_elo,
        raising=False,
    )
    monkeypatch.setattr(
        runtime,
        "_drop_delayed_match",
        lambda match_key, reason="": dropped.append({"match_key": match_key, "reason": reason}) or True,
        raising=False,
    )

    updates = runtime._finalize_orphaned_live_elo_series(set())

    assert len(updates) == 1
    assert finalize_calls == [
        {
            "series_key": "425561",
            "series_url": "dltv.org/matches/425561/pipsqueak4-vs-l1ga-team-premier-series-play-in",
            "first_team_score": 0,
            "second_team_score": 2,
        }
    ]
    assert dropped == [
        {
            "match_key": "dltv.org/matches/425561/pipsqueak4-vs-l1ga-team-premier-series-play-in.1",
            "reason": "orphan_series_finished_live_elo_applied",
        }
    ]


def test_stale_duplicate_live_map_payload_is_not_added_to_map_id_check(tmp_path, monkeypatch) -> None:
    html = """
    <div class="head">
      <div class="event__info-info__time">draft...</div>
    </div>
    <div class="body">
      <div class="match__item-team__score">0</div>
      <div class="match__item-team__score">1</div>
      <a href="https://dltv.org/matches/425633/virtuspro-vs-nigma-esl-one-birmingham-2026"></a>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    head = soup.find("div", class_="head")
    body = soup.find("div", class_="body")
    assert head is not None and body is not None

    progress_path = tmp_path / "live_elo_progress.json"
    progress_path.write_text(
        json.dumps(
            {
                "pending_series": {},
                "applied_maps": {
                    "dltv.org/matches/425633/virtuspro-vs-nigma-esl-one-birmingham-2026.0": {
                        "series_key": "425633",
                        "series_url": "dltv.org/matches/425633/virtuspro-vs-nigma-esl-one-birmingham-2026",
                        "winner_slot": "second",
                        "radiant_win": False,
                        "applied_at": 1774215612,
                        "match_id": 8740039655,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(runtime, "_elo_live_default_progress_path", progress_path, raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda *_args, **_kwargs: None)

    add_url_calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, reason="unspecified", details=None: add_url_calls.append(
            {"url": url, "reason": reason, "details": details}
        ),
    )

    page_html = "<html><script>$.get('/live/test-stale.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "match_id": 8740039655,
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Virtus.pro", "team_id": 2, "id": 2},
            "second_team": {"title": "Nigma Galaxy", "team_id": 5124, "id": 5124},
            "series": {"id": 425633},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 2},
            "dire_team": {"team_id": 5124},
            "radiant_series_wins": 0,
            "dire_series_wins": 1,
            "league_id": 19422,
        },
        "radiant_lead": -41020.0,
        "game_time": 2219.0,
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [2] if team_id_calls["count"] == 1 else [5124]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        runtime,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: 1)

    parse_called = {"value": False}

    def _should_not_parse(*_args, **_kwargs):
        parse_called["value"] = True
        raise AssertionError("parse_draft_and_positions must not run for stale duplicate payload")

    monkeypatch.setattr(runtime, "parse_draft_and_positions", _should_not_parse)

    runtime.check_head(
        heads=[head],
        bodies=[body],
        i=0,
        maps_data=set(),
        return_status=None,
    )

    assert parse_called["value"] is False
    assert add_url_calls == []


def test_stale_duplicate_live_map_payload_is_not_added_to_map_id_check_for_later_bo5_map(tmp_path, monkeypatch) -> None:
    html = """
    <div class="head">
      <div class="event__info-info__time">draft...</div>
    </div>
    <div class="body">
      <div class="match__item-team__score">2</div>
      <div class="match__item-team__score">1</div>
      <a href="https://dltv.org/matches/425999/team-a-vs-team-b-bo5-final"></a>
    </div>
    """
    soup = BeautifulSoup(html, "lxml")
    head = soup.find("div", class_="head")
    body = soup.find("div", class_="body")
    assert head is not None and body is not None

    progress_path = tmp_path / "live_elo_progress.json"
    progress_path.write_text(
        json.dumps(
            {
                "pending_series": {},
                "applied_maps": {
                    "dltv.org/matches/425999/team-a-vs-team-b-bo5-final.2": {
                        "series_key": "425999",
                        "series_url": "dltv.org/matches/425999/team-a-vs-team-b-bo5-final",
                        "winner_slot": "second",
                        "radiant_win": False,
                        "applied_at": 1774215612,
                        "match_id": 999000333,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(runtime, "_elo_live_default_progress_path", progress_path, raising=False)
    monkeypatch.setattr(runtime, "BOOKMAKER_PREFETCH_ENABLED", False, raising=False)
    monkeypatch.setattr(runtime, "FORCE_ODDS_SIGNAL_TEST", False, raising=False)
    monkeypatch.setattr(runtime, "_ensure_delayed_sender_started", lambda: None)
    monkeypatch.setattr(runtime, "_is_url_processed", lambda _url: False)
    monkeypatch.setattr(runtime, "_drop_delayed_match", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_skip_dispatch_for_processed_url", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(runtime, "_acquire_signal_send_slot", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runtime, "_release_signal_send_slot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_mark_url_processed", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "_log_bookmaker_source_snapshot", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runtime, "send_message", lambda *_args, **_kwargs: None)

    add_url_calls: List[Dict[str, Any]] = []
    monkeypatch.setattr(
        runtime,
        "add_url",
        lambda url, reason="unspecified", details=None: add_url_calls.append(
            {"url": url, "reason": reason, "details": details}
        ),
    )

    page_html = "<html><script>$.get('/live/test-stale-bo5.json')</script></html>"
    monkeypatch.setattr(
        runtime,
        "make_request_with_retry",
        lambda *_args, **_kwargs: _FakeTextResponse(page_html, status_code=200),
    )

    live_data = {
        "match_id": 999000333,
        "fast_picks": [1],
        "db": {
            "first_team": {"is_radiant": True, "title": "Team A", "team_id": 1001, "id": 1001},
            "second_team": {"title": "Team B", "team_id": 2002, "id": 2002},
            "series": {"id": 425999},
            "scores": {"first_team": 2, "second_team": 1},
        },
        "live_league_data": {
            "match": {},
            "radiant_team": {"team_id": 1001},
            "dire_team": {"team_id": 2002},
            "radiant_series_wins": 2,
            "dire_series_wins": 1,
            "league_id": 19422,
        },
        "radiant_lead": -12000.0,
        "game_time": 1800.0,
    }
    monkeypatch.setattr(
        runtime.requests,
        "get",
        lambda *_args, **_kwargs: _FakeJsonResponse(live_data, status_code=200),
    )

    team_id_calls = {"count": 0}

    def _extract_candidate_team_ids(*_args, **_kwargs):
        team_id_calls["count"] += 1
        return [1001] if team_id_calls["count"] == 1 else [2002]

    monkeypatch.setattr(runtime, "_extract_candidate_team_ids", _extract_candidate_team_ids)
    monkeypatch.setattr(
        runtime,
        "_ensure_known_team_or_add_to_tier2",
        lambda team_ids, _team_name, _match_key: (True, int(team_ids[0])),
    )
    monkeypatch.setattr(runtime, "_determine_star_signal_match_tier", lambda *_args, **_kwargs: 1)

    parse_called = {"value": False}

    def _should_not_parse(*_args, **_kwargs):
        parse_called["value"] = True
        raise AssertionError("parse_draft_and_positions must not run for stale duplicate payload on later BO5 map")

    monkeypatch.setattr(runtime, "parse_draft_and_positions", _should_not_parse)

    runtime.check_head(
        heads=[head],
        bodies=[body],
        i=0,
        maps_data=set(),
        return_status=None,
    )

    assert parse_called["value"] is False
    assert add_url_calls == []
