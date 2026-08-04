import io
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from base.swarm_telegram_bot import (
    OmniRouteClient, OmniRouteError, OrchestratorProcess, Settings, SwarmBot, account_alias, redact,
)


class Response:
    def __init__(self, payload): self.payload = payload
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def read(self): return json.dumps(self.payload).encode()


class FakeHTTP:
    def __init__(self):
        self.calls = []
        self.connections = [{"id": "real-sensitive-id", "isActive": True, "provider": "codex", "status": "ready"}]
    def __call__(self, req, timeout):
        body = json.loads(req.data) if req.data else None
        self.calls.append((req.method, req.full_url, body))
        if req.full_url.endswith("/api/version"): return Response({"version": "3.8.46", "schemaVersion": 1})
        if req.full_url.endswith("/api/providers") and req.method == "GET": return Response({"connections": self.connections})
        if req.method in {"PATCH", "DELETE"}: return Response({"success": True})
        if req.full_url.endswith("/test"): return Response({"success": True})
        return Response({"success": True})


def client(fake=None):
    fake = fake or FakeHTTP()
    return OmniRouteClient("http://127.0.0.1:20128", "management-secret", opener=fake), fake


def test_accounts_are_aliased_and_mutations_use_internal_id():
    omni, http = client()
    rows = omni.accounts()
    assert rows[0]["alias"] == account_alias("real-sensitive-id")
    assert "real-sensitive-id" not in str(rows)
    omni.set_active(rows[0]["alias"], False)
    assert http.calls[-1][2] == {"ids": ["real-sensitive-id"], "isActive": False}


def test_version_mismatch_fails_closed_before_accounts():
    def fake(req, timeout): return Response({"version": "3.9.0", "schemaVersion": 1})
    omni = OmniRouteClient("http://127.0.0.1:1", "x", opener=fake)
    with pytest.raises(OmniRouteError, match="mismatch"):
        omni.accounts()


class FakeOrchestrator:
    def __init__(self): self.tasks = []; self.cancelled = False
    def run(self, task): self.tasks.append(task)
    def status(self): return "running"
    def cancel(self): self.cancelled = True; return True


def test_dm_allowlist_and_run_commands():
    omni, _ = client()
    proc = FakeOrchestrator()
    bot = SwarmBot(42, omni, proc)
    assert bot.handle(41, "private", "/run nope") is None
    assert bot.handle(42, "group", "/run nope") is None
    assert "запущена" in bot.handle(42, "private", "/run fix tests")
    assert proc.tasks == ["fix tests"]
    assert bot.handle(42, "private", "/status") == "running"
    assert "запрошена" in bot.handle(42, "private", "/cancel")


def test_account_disable_requires_confirmation_and_hides_raw_id():
    omni, http = client()
    bot = SwarmBot(42, omni, FakeOrchestrator())
    alias = account_alias("real-sensitive-id")
    reply = bot.handle(42, "private", "/account_disable " + alias)
    assert "real-sensitive-id" not in reply
    code = reply.rsplit(" ", 2)[-2]
    assert bot.handle(42, "private", "/confirm wrong").startswith("Подтверждение недействительно")
    reply = bot.handle(42, "private", "/account_disable " + alias)
    code = reply.split("/confirm ", 1)[1].split()[0]
    assert bot.handle(42, "private", "/confirm " + code) == "Готово."
    assert http.calls[-1][0] == "PATCH"


def test_redaction():
    value = redact("alice@example.com token=abc 123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZ123")
    assert "alice" not in value and "abc" not in value and "123456789:" not in value


def test_settings_reject_non_loopback(monkeypatch):
    monkeypatch.setenv("SWARM_TELEGRAM_BOT_TOKEN", "secret")
    monkeypatch.setenv("SWARM_TELEGRAM_ALLOWED_USER_ID", "42")
    monkeypatch.setenv("SWARM_ORCHESTRATOR_COMMAND", '["runner"]')
    monkeypatch.setenv("OMNIROUTE_MANAGEMENT_KEY", "key")
    monkeypatch.setenv("OMNIROUTE_MANAGEMENT_URL", "https://remote.example")
    with pytest.raises(Exception, match="loopback"):
        Settings.from_env()


class FakeStdin(io.StringIO):
    def close(self):
        self.saved = self.getvalue()


class FakePopen:
    def __init__(self, *args, **kwargs):
        self.pid = 999; self.stdin = FakeStdin(); self.code = None
    def poll(self): return self.code


def test_orchestrator_busy_lock_and_pgid_cancel():
    created = []
    def factory(*args, **kwargs):
        p = FakePopen(); created.append((p, kwargs)); return p
    runner = OrchestratorProcess(("runner",), popen=factory)
    runner.run("task")
    assert created[0][0].stdin.saved == "task"
    assert created[0][1]["start_new_session"] is True
    with pytest.raises(RuntimeError, match="busy"):
        runner.run("second")
    with patch("os.killpg") as killpg:
        assert runner.cancel()
        killpg.assert_called_once_with(999, 15)
        created[0][0].code = 0


def test_oauth_relays_only_openai_device_url_and_imports_without_exposing_auth(tmp_path):
    omni, http = client()
    proc = FakeOrchestrator()
    bot = SwarmBot(42, omni, proc, ("codex", "login", "--device-auth"))

    class DeviceProcess:
        pid = 1001
        returncode = None
        def __init__(self, command, **kwargs):
            self.home = kwargs["env"]["CODEX_HOME"]
            kwargs["stdout"].write(
                "Visit https://auth.openai.com/codex/device?private=hidden code: ABCD-EFGH "
                "https://evil.example/leak?token=bad"
            )
            kwargs["stdout"].flush()
        def poll(self): return self.returncode

    made = []
    def factory(*args, **kwargs):
        item = DeviceProcess(*args, **kwargs); made.append(item); return item

    with patch("subprocess.Popen", factory):
        prepare = bot.handle(42, "private", "/account_add")
        code = prepare.split("/confirm ", 1)[1].split()[0]
        public = bot.handle(42, "private", "/confirm " + code)
        assert "https://auth.openai.com/codex/device" in public
        assert "private=hidden" not in public and "evil.example" not in public
        assert "ABCD-EFGH" in public
        auth = {"tokens": {"access_token": "never-show-this"}}
        (Path(made[0].home) / "auth.json").write_text(json.dumps(auth))
        made[0].returncode = 0
        assert "импортирован" in bot.handle(42, "private", "/account_add")
        assert http.calls[-1][2] == {"accounts": auth}
