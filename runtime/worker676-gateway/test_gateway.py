import importlib.util
import io
import sys
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("worker676_gateway", HERE / "gateway.py")
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class Worker676GatewayTests(unittest.TestCase):
    def test_fable_effort_high_is_forwarded_to_codex(self):
        runner = MODULE.CodexRunner(
            "/usr/local/bin/codex",
            Path("/root/main"),
            "test",
            {"claude-fable-5": "high"},
        )
        model = MODULE.Model("claude-fable-5", "fable", 1_000_000)
        command = []

        class FakeProcess:
            returncode = 0
            pid = 123

            def __init__(self, args, **kwargs):
                command.extend(args)
                self.stdout = iter(
                    [
                        '{"type":"thread.started","thread_id":"thread-1"}\n',
                        '{"type":"item.completed","item":{"type":"agent_message","text":"ok"}}\n',
                    ]
                )
                self.stderr = io.StringIO("")

            def wait(self):
                return 0

        original = MODULE.subprocess.Popen
        MODULE.subprocess.Popen = FakeProcess
        try:
            output, thread = runner.run("run", model, "prompt", None, [])
        finally:
            MODULE.subprocess.Popen = original
        self.assertEqual((output, thread), ("ok", "thread-1"))
        self.assertIn('model_reasoning_effort="high"', command)


if __name__ == "__main__":
    unittest.main()
