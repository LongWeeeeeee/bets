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
            sent_keys = getattr(module, "_SENT_SIGNAL_DEDUP_KEYS", None)
            if isinstance(sent_keys, set):
                sent_keys.clear()
    yield


@pytest.fixture(autouse=True)
def _isolate_winline_odds_history(tmp_path, monkeypatch):
    """Не даёт тестам писать в БОЕВОЙ архив котировок.

    `WINLINE_ODDS_HISTORY_PATH` в поллере вычисляется НА ИМПОРТЕ, поэтому одной
    переменной среды мало: если модуль уже загружен, константа в нём осталась
    боевой. Отсюда правим и атрибут модуля.

    Зачем. В архиве накопилось 463 записи с фиктивным временем (1700000000 и
    соседние круглые числа) — это тестовые прогоны, попавшие в файл, который
    мы собираемся джойнить с корпусом по `match_id`. Отличить их можно только
    по неправдоподобному `wall`, то есть постфактум и вручную.
    """
    target = str(tmp_path / "winline_odds_history.jsonl")
    monkeypatch.setenv("WINLINE_ODDS_HISTORY_PATH", target)
    for name, module in list(sys.modules.items()):
        if name.rsplit(".", 1)[-1] == "winline_current_map_odds_poller":
            if hasattr(module, "WINLINE_ODDS_HISTORY_PATH"):
                monkeypatch.setattr(module, "WINLINE_ODDS_HISTORY_PATH", target,
                                    raising=False)
