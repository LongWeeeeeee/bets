import sys

import pytest


@pytest.fixture(autouse=True)
def _isolate_sent_signal_fingerprints(tmp_path, monkeypatch):
    """Изолирует межинстансный дедуп сигналов от общего state-файла.

    Без изоляции отпечатки тестовых матчей (generic team names + замороженное
    time.time()) копятся в ~/.local/state/ingame/sent_signal_fingerprints.json
    и блокируют отправку в последующих тестах/прогонах.
    """
    monkeypatch.setenv(
        "SENT_SIGNAL_FINGERPRINT_PATH",
        str(tmp_path / "sent_signal_fingerprints.json"),
    )
    for module_name, module in list(sys.modules.items()):
        if module_name.rsplit(".", 1)[-1] == "cyberscore_try":
            registry = getattr(module, "_SIGNAL_DEDUP_FINGERPRINTS", None)
            if isinstance(registry, dict):
                registry.clear()
    yield
